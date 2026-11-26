/*
Copyright 2015 The Kubernetes Authors.

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

package apps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"k8s.io/client-go/tools/cache"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	watch "k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	extensionsinternal "k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/controller/daemon"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edaemonset "k8s.io/kubernetes/test/e2e/framework/daemonset"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2eresource "k8s.io/kubernetes/test/e2e/framework/resource"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// 这里不能是 5 的倍数，否则会跟 node 状态更新撞上
	// every 5 seconds. See https://github.com/kubernetes/kubernetes/pull/14915.
	dsRetryPeriod  = 1 * time.Second
	dsRetryTimeout = 5 * time.Minute

	daemonsetLabelPrefix = "daemonset-"
	daemonsetNameLabel   = daemonsetLabelPrefix + "name"
	daemonsetColorLabel  = daemonsetLabelPrefix + "color"
)

// NamespaceNodeSelectors 是注解 scheduler.alpha.kubernetes.io/node-selector 的键，用于为
// namespace 分配 node selector 标签
var NamespaceNodeSelectors = []string{"scheduler.alpha.kubernetes.io/node-selector"}

var nonTerminalPhaseSelector = func() labels.Selector {
	var reqs []labels.Requirement
	for _, phase := range []v1.PodPhase{v1.PodFailed, v1.PodSucceeded} {
		req, _ := labels.NewRequirement("status.phase", selection.NotEquals, []string{string(phase)})
		reqs = append(reqs, *req)
	}
	selector := labels.NewSelector()
	return selector.Add(reqs...)
}()

type updateDSFunc func(*appsv1.DaemonSet)

// updateDaemonSetWithRetries 用给定的 applyUpdate 函数更新 daemonset，
// 直到成功或超时。
func updateDaemonSetWithRetries(ctx context.Context, c clientset.Interface, namespace, name string, applyUpdate updateDSFunc) (ds *appsv1.DaemonSet, err error) {
	daemonsets := c.AppsV1().DaemonSets(namespace)
	var updateErr error
	pollErr := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		if ds, err = daemonsets.Get(ctx, name, metav1.GetOptions{}); err != nil {
			return false, err
		}
		// 应用更新，然后尝试推送到 apiserver。
		applyUpdate(ds)
		if ds, err = daemonsets.Update(ctx, ds, metav1.UpdateOptions{}); err == nil {
			framework.Logf("Updating DaemonSet %s", name)
			return true, nil
		}
		updateErr = err
		return false, nil
	})
	if wait.Interrupted(pollErr) {
		pollErr = fmt.Errorf("couldn't apply the provided updated to DaemonSet %q: %v", name, updateErr)
	}
	return ds, pollErr
}

// 本测试必须串行运行，因为它假设 Daemon Set 的 pod
// 总会被调度。如果并行运行其他测试，这个假设可能不成立。
// 将来如果有驱逐模型让 DS controller 能踢掉其他 pod 腾出空间，
// 并行运行或许可行。
// See https://issues.k8s.io/21767 for more details
var _ = SIGDescribe("Daemon set", framework.WithSerial(), func() {
	var f *framework.Framework

	ginkgo.AfterEach(func(ctx context.Context) {
		// 清理
		daemonsets, err := f.ClientSet.AppsV1().DaemonSets(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "unable to dump DaemonSets")
		if daemonsets != nil && len(daemonsets.Items) > 0 {
			for _, ds := range daemonsets.Items {
				ginkgo.By(fmt.Sprintf("Deleting DaemonSet %q", ds.Name))
				framework.ExpectNoError(e2eresource.DeleteResourceAndWaitForGC(ctx, f.ClientSet, extensionsinternal.Kind("DaemonSet"), f.Namespace.Name, ds.Name))
				err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnNoNodes(f, &ds))
				framework.ExpectNoError(err, "error waiting for daemon pod to be reaped")
			}
		}
		if daemonsets, err := f.ClientSet.AppsV1().DaemonSets(f.Namespace.Name).List(ctx, metav1.ListOptions{}); err == nil {
			framework.Logf("daemonset: %s", runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...), daemonsets))
		} else {
			framework.Logf("unable to dump daemonsets: %v", err)
		}
		if pods, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{}); err == nil {
			framework.Logf("pods: %s", runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...), pods))
		} else {
			framework.Logf("unable to dump pods: %v", err)
		}
		err = clearDaemonSetNodeLabels(ctx, f.ClientSet)
		framework.ExpectNoError(err)
	})

	f = framework.NewDefaultFramework("daemonsets")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	image := AgnhostImage
	dsName := "daemon-set"

	var ns string
	var c clientset.Interface

	ginkgo.BeforeEach(func(ctx context.Context) {
		ns = f.Namespace.Name

		c = f.ClientSet

		updatedNS, err := patchNamespaceAnnotations(ctx, c, ns)
		framework.ExpectNoError(err)

		ns = updatedNS.Name

		err = clearDaemonSetNodeLabels(ctx, c)
		framework.ExpectNoError(err)
	})

	/*
	  发布版本：v1.10
	  测试名称：DaemonSet-Creation
	  描述：符合规范的 Kubernetes 发行版必须支持创建 DaemonSet。DaemonSet
	  Pod 被删除后，DaemonSet controller 必须创建替代 Pod。
	*/
	framework.ConformanceIt("should run and stop simple daemon", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}

		ginkgo.By(fmt.Sprintf("Creating simple DaemonSet %q", dsName))
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, newDaemonSet(dsName, image, label), metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("Stop a daemon pod, check that the daemon pod is revived.")
		podList := listDaemonPods(ctx, c, ns, label)
		pod := podList.Items[0]
		err = c.CoreV1().Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to revive")
	})

	/*
	  发布版本：v1.10
	  测试名称：DaemonSet-NodeSelection
	  描述：符合规范的 Kubernetes 发行版必须支持通过 label selector
	  选择 DaemonSet Pod 的 node。
	*/
	framework.ConformanceIt("should run and stop complex daemon", func(ctx context.Context) {
		complexLabel := map[string]string{daemonsetNameLabel: dsName}
		nodeSelector := map[string]string{daemonsetColorLabel: "blue"}
		framework.Logf("Creating daemon %q with a node selector", dsName)
		ds := newDaemonSet(dsName, image, complexLabel)
		ds.Spec.Template.Spec.NodeSelector = nodeSelector
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Initially, daemon pods should not be running on any nodes.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnNoNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pods to be running on no nodes")

		ginkgo.By("Change node label to blue, check that daemon pod is launched.")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		newNode, err := setDaemonSetNodeLabels(ctx, c, node.Name, nodeSelector)
		framework.ExpectNoError(err, "error setting labels on node")
		daemonSetLabels, _ := separateDaemonSetNodeLabels(newNode.Labels)
		gomega.Expect(daemonSetLabels).To(gomega.HaveLen(1))
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, e2edaemonset.CheckDaemonPodOnNodes(f, ds, []string{newNode.Name}))
		framework.ExpectNoError(err, "error waiting for daemon pods to be running on new nodes")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("Update the node label to green, and wait for daemons to be unscheduled")
		nodeSelector[daemonsetColorLabel] = "green"
		greenNode, err := setDaemonSetNodeLabels(ctx, c, node.Name, nodeSelector)
		framework.ExpectNoError(err, "error removing labels on node")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnNoNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to not be running on nodes")

		ginkgo.By("Update DaemonSet node selector to green, and change its update strategy to RollingUpdate")
		patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"%s":"%s"}}},"updateStrategy":{"type":"RollingUpdate"}}}`,
			daemonsetColorLabel, greenNode.Labels[daemonsetColorLabel])
		ds, err = c.AppsV1().DaemonSets(ns).Patch(ctx, dsName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
		framework.ExpectNoError(err, "error patching daemon set")
		daemonSetLabels, _ = separateDaemonSetNodeLabels(greenNode.Labels)
		gomega.Expect(daemonSetLabels).To(gomega.HaveLen(1))
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, e2edaemonset.CheckDaemonPodOnNodes(f, ds, []string{greenNode.Name}))
		framework.ExpectNoError(err, "error waiting for daemon pods to be running on new nodes")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)
	})

	// 暂不将该测试纳入一致性测试，需等 DaemonSet 调度逻辑迁移到
	// 默认 scheduler 的方案确定后再说。
	ginkgo.It("should run and stop complex daemon with node affinity", func(ctx context.Context) {
		complexLabel := map[string]string{daemonsetNameLabel: dsName}
		nodeSelector := map[string]string{daemonsetColorLabel: "blue"}
		framework.Logf("Creating daemon %q with a node affinity", dsName)
		ds := newDaemonSet(dsName, image, complexLabel)
		ds.Spec.Template.Spec.Affinity = &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      daemonsetColorLabel,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{nodeSelector[daemonsetColorLabel]},
								},
							},
						},
					},
				},
			},
		}
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Initially, daemon pods should not be running on any nodes.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnNoNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pods to be running on no nodes")

		ginkgo.By("Change node label to blue, check that daemon pod is launched.")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		newNode, err := setDaemonSetNodeLabels(ctx, c, node.Name, nodeSelector)
		framework.ExpectNoError(err, "error setting labels on node")
		daemonSetLabels, _ := separateDaemonSetNodeLabels(newNode.Labels)
		gomega.Expect(daemonSetLabels).To(gomega.HaveLen(1))
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, e2edaemonset.CheckDaemonPodOnNodes(f, ds, []string{newNode.Name}))
		framework.ExpectNoError(err, "error waiting for daemon pods to be running on new nodes")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("Remove the node label and wait for daemons to be unscheduled")
		_, err = setDaemonSetNodeLabels(ctx, c, node.Name, map[string]string{})
		framework.ExpectNoError(err, "error removing labels on node")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnNoNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to not be running on nodes")
	})

	/*
	  发布版本：v1.10
	  测试名称：DaemonSet-FailedPodCreation
	  描述：符合规范的 Kubernetes 发行版必须在 DaemonSet Pod 失败时创建新的 Pod。
	*/
	framework.ConformanceIt("should retry creating failed daemon pods", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}

		ginkgo.By(fmt.Sprintf("Creating a simple DaemonSet %q", dsName))
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, newDaemonSet(dsName, image, label), metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("Set a daemon pod's phase to 'Failed', check that the daemon pod is revived.")
		podList := listDaemonPods(ctx, c, ns, label)
		pod := podList.Items[0]
		pod.ResourceVersion = ""
		pod.Status.Phase = v1.PodFailed
		_, err = c.CoreV1().Pods(ns).UpdateStatus(ctx, &pod, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "error failing a daemon pod")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to revive")

		ginkgo.By("Wait for the failed daemon pod to be completely deleted.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, waitFailedDaemonPodDeleted(c, &pod))
		framework.ExpectNoError(err, "error waiting for the failed daemon pod to be completely deleted")
	})

	// 这个测试不应纳入一致性测试。等 extensions/v1beta1 和
	// apps/v1beta1 移除后，我们会考虑弃用 OnDelete。
	ginkgo.It("should not update pod when spec was updated and update strategy is OnDelete", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}

		framework.Logf("Creating simple daemon set %s", dsName)
		ds := newDaemonSet(dsName, image, label)
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 1)
		first := curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		firstHash := first.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(first.Revision).To(gomega.Equal(int64(1)))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), firstHash)

		ginkgo.By("Update daemon pods image.")
		patch := getDaemonSetImagePatch(ds.Spec.Template.Spec.Containers[0].Name, PrevAgnhostImage)
		ds, err = c.AppsV1().DaemonSets(ns).Patch(ctx, dsName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods images aren't updated.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkDaemonPodsImageAndAvailability(c, ds, image, 0))
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods are still running on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 2)
		cur := curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		gomega.Expect(cur.Revision).To(gomega.Equal(int64(2)))
		gomega.Expect(cur.Labels).NotTo(gomega.HaveKeyWithValue(appsv1.DefaultDaemonSetUniqueLabelKey, firstHash))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), firstHash)
	})

	/*
	  发布版本：v1.10
	  测试名称：DaemonSet-RollingUpdate
	  描述：符合规范的 Kubernetes 发行版必须支持 DaemonSet 滚动更新。
	*/
	framework.ConformanceIt("should update pod when spec was updated and update strategy is RollingUpdate", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}

		framework.Logf("Creating simple daemon set %s", dsName)
		ds := newDaemonSet(dsName, image, label)
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType}
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(ds).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 1)
		cur := curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		hash := cur.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(cur.Revision).To(gomega.Equal(int64(1)))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), hash)

		ginkgo.By("Update daemon pods image.")
		oldDsResourceVersion := ds.ResourceVersion
		patch := getDaemonSetImagePatch(ds.Spec.Template.Spec.Containers[0].Name, PrevAgnhostImage)
		ds, err = c.AppsV1().DaemonSets(ns).Patch(ctx, dsName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(resourceversion.CompareResourceVersion(oldDsResourceVersion, ds.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		// 滚动升级的完成时间与集群节点数成正比。
		// 先获取节点数，再相应设置超时。
		nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		nodeCount := len(nodes.Items)
		retryTimeout := dsRetryTimeout + time.Duration(nodeCount*30)*time.Second

		ginkgo.By("Check that daemon pods images are updated.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, retryTimeout, true, checkDaemonPodsImageAndAvailability(c, ds, PrevAgnhostImage, 1))
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods are still running on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 2)
		cur = curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		hash = cur.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(cur.Revision).To(gomega.Equal(int64(2)))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), hash)
	})

	/*
	  发布版本：v1.10
	  测试名称：DaemonSet-Rollback
	  描述：符合规范的 Kubernetes 发行版必须支持自动且中断最小的
	  DaemonSet 更新回滚。
	*/
	framework.ConformanceIt("should rollback without unnecessary restarts", func(ctx context.Context) {
		schedulableNodes, err := e2enode.GetReadySchedulableNodes(ctx, c)
		framework.ExpectNoError(err)
		gomega.Expect(len(schedulableNodes.Items)).To(gomega.BeNumerically(">", 1), "Conformance test suite needs a cluster with at least 2 nodes.")
		framework.Logf("Create a RollingUpdate DaemonSet")
		label := map[string]string{daemonsetNameLabel: dsName}
		ds := newDaemonSet(dsName, image, label)
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType}
		ds, err = c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		framework.Logf("Check that daemon pods launch on every node of the cluster")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		framework.Logf("Update the DaemonSet to trigger a rollout")
		// 这里使用一个不存在的镜像，以确保它不会完成
		newImage := "foo:non-existent"
		newDS, err := updateDaemonSetWithRetries(ctx, c, ns, ds.Name, func(update *appsv1.DaemonSet) {
			update.Spec.Template.Spec.Containers[0].Image = newImage
		})
		framework.ExpectNoError(err)

		// 确保正处于滚动更新中
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkAtLeastOneNewPod(c, ns, label, newImage))
		framework.ExpectNoError(err)

		pods := listDaemonPods(ctx, c, ns, label)
		var existingPods, newPods []*v1.Pod
		for i := range pods.Items {
			pod := pods.Items[i]
			image := pod.Spec.Containers[0].Image
			switch image {
			case ds.Spec.Template.Spec.Containers[0].Image:
				existingPods = append(existingPods, &pod)
			case newDS.Spec.Template.Spec.Containers[0].Image:
				newPods = append(newPods, &pod)
			default:
				framework.Failf("unexpected pod found, image = %s", image)
			}
		}
		schedulableNodes, err = e2enode.GetReadySchedulableNodes(ctx, c)
		framework.ExpectNoError(err)
		if len(schedulableNodes.Items) < 2 {
			gomega.Expect(existingPods).To(gomega.BeEmpty())
		} else {
			gomega.Expect(existingPods).NotTo(gomega.BeEmpty())
		}
		gomega.Expect(newPods).NotTo(gomega.BeEmpty())

		framework.Logf("Roll back the DaemonSet before rollout is complete")
		rollbackDS, err := updateDaemonSetWithRetries(ctx, c, ns, ds.Name, func(update *appsv1.DaemonSet) {
			update.Spec.Template.Spec.Containers[0].Image = image
		})
		framework.ExpectNoError(err)

		framework.Logf("Make sure DaemonSet rollback is complete")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkDaemonPodsImageAndAvailability(c, rollbackDS, image, 1))
		framework.ExpectNoError(err)

		// 回滚完成后，将当前 pod 与滚动更新期间的旧 pod 比较，确保它们没有被重启
		pods = listDaemonPods(ctx, c, ns, label)
		rollbackPods := map[string]bool{}
		for _, pod := range pods.Items {
			rollbackPods[pod.Name] = true
		}
		for _, pod := range existingPods {
			if !rollbackPods[pod.Name] {
				framework.Failf("unexpected pod %s be restarted", pod.Name)
			}
		}
	})

	// TODO: 该功能晋级后，这个测试预计会被提升为一致性测试
	ginkgo.It("should surge pods onto nodes when spec was updated and update strategy is RollingUpdate", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}

		framework.Logf("Creating surge daemon set %s", dsName)
		maxSurgeOverlap := 60 * time.Second
		maxSurge := 1
		surgePercent := intstr.FromString("20%")
		zero := intstr.FromInt32(0)
		oldVersion := "1"
		ds := newDaemonSet(dsName, image, label)
		ds.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{
			{Name: "VERSION", Value: oldVersion},
		}
		// 延迟关闭 15 秒，让容器在时间上有重叠
		ds.Spec.Template.Spec.Containers[0].Lifecycle = &v1.Lifecycle{
			PreStop: &v1.LifecycleHandler{
				Exec: &v1.ExecAction{
					Command: []string{"/bin/sh", "-c", "sleep 15"},
				},
			},
		}
		// 使用可强制失败的 readiness 探针（修改 /var/tmp/ready 的内容）
		ds.Spec.Template.Spec.Containers[0].ReadinessProbe = &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				Exec: &v1.ExecAction{
					Command: []string{"/bin/sh", "-ec", `touch /var/tmp/ready; [[ "$( cat /var/tmp/ready )" == "" ]]`},
				},
			},
			InitialDelaySeconds: 7,
			PeriodSeconds:       3,
			SuccessThreshold:    1,
			FailureThreshold:    1,
		}
		// 使用简单的 surge 策略
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{
			Type: appsv1.RollingUpdateDaemonSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDaemonSet{
				MaxUnavailable: &zero,
				MaxSurge:       &surgePercent,
			},
		}
		// 删除旧 pod 前，新 pod 必须至少 ready 10 秒
		ds.Spec.MinReadySeconds = 10

		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 1)
		cur := curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		hash := cur.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(cur.Revision).To(gomega.Equal(int64(1)))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), hash)

		newVersion := "2"
		ginkgo.By("Update daemon pods environment var")
		patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"containers":[{"name":"%s","env":[{"name":"VERSION","value":"%s"}]}]}}}}`, ds.Spec.Template.Spec.Containers[0].Name, newVersion)
		ds, err = c.AppsV1().DaemonSets(ns).Patch(ctx, dsName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
		framework.ExpectNoError(err)

		// 滚动升级的完成时间与集群节点数成正比。
		// 先获取节点数，再相应设置超时。
		nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		nodeCount := len(nodes.Items)
		// 通过随机终止 pod 干扰 daemonset 的更新进度。
		randomPodTerminationTimeout := 5 * time.Minute
		retryTimeout := dsRetryTimeout + randomPodTerminationTimeout + time.Duration(nodeCount*30)*time.Second

		ginkgo.By("Check that daemon pods surge and invariants are preserved during that rollout")
		nodeToAgeOfOldPod := make(map[string]map[string]time.Time)
		deliberatelyDeletedPods := sets.NewString()
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, retryTimeout, true, func(ctx context.Context) (bool, error) {
			podList, err := c.CoreV1().Pods(ds.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			pods := podList.Items

			var buf bytes.Buffer
			pw := tabwriter.NewWriter(&buf, 1, 1, 1, ' ', 0)
			fmt.Fprint(pw, "Node\tVersion\tName\tUID\tDeleted\tReady\n")

			now := time.Now()
			podUIDs := sets.NewString()
			deletedPodUIDs := sets.NewString()
			nodes := sets.NewString()
			versions := sets.NewString()
			nodesToVersions := make(map[string]map[string]int)
			nodesToDeletedVersions := make(map[string]map[string]int)
			var surgeCount, newUnavailableCount, newDeliberatelyDeletedCount, oldUnavailableCount, nodesWithoutOldVersion int
			for _, pod := range pods {
				if !metav1.IsControlledBy(&pod, ds) {
					continue
				}
				nodeName := pod.Spec.NodeName
				nodes.Insert(nodeName)
				podVersion := pod.Spec.Containers[0].Env[0].Value
				if pod.DeletionTimestamp != nil {
					if !deliberatelyDeletedPods.Has(string(pod.UID)) {
						versions := nodesToDeletedVersions[nodeName]
						if versions == nil {
							versions = make(map[string]int)
							nodesToDeletedVersions[nodeName] = versions
						}
						versions[podVersion]++
					}
				} else {
					versions := nodesToVersions[nodeName]
					if versions == nil {
						versions = make(map[string]int)
						nodesToVersions[nodeName] = versions
					}
					versions[podVersion]++
				}

				ready := podutil.IsPodAvailable(&pod, ds.Spec.MinReadySeconds, metav1.Now())
				if podVersion == newVersion {
					surgeCount++
					if !ready || pod.DeletionTimestamp != nil {
						if deliberatelyDeletedPods.Has(string(pod.UID)) {
							newDeliberatelyDeletedCount++
						}
						newUnavailableCount++
					}
				} else {
					if !ready || pod.DeletionTimestamp != nil {
						oldUnavailableCount++
					}
				}
				fmt.Fprintf(pw, "%s\t%s\t%s\t%s\t%t\t%t\n", pod.Spec.NodeName, podVersion, pod.Name, pod.UID, pod.DeletionTimestamp != nil, ready)
			}

			// 按节点打印稳定的 pod 排序列表，便于调试
			pw.Flush()
			lines := strings.Split(buf.String(), "\n")
			lines = lines[:len(lines)-1]
			sort.Strings(lines[1:])
			for _, line := range lines {
				framework.Logf("%s", line)
			}

			// 如果同时存在新旧版本的 pod，记录时间戳
			deletedPerNode := make(map[string]int)
			for _, pod := range pods {
				if !metav1.IsControlledBy(&pod, ds) {
					continue
				}
				// 忽略已删除的 pod
				if pod.DeletionTimestamp != nil {
					deletedPodUIDs.Insert(string(pod.UID))
					if !deliberatelyDeletedPods.Has(string(pod.UID)) {
						deletedPerNode[pod.Spec.NodeName]++
					}
					continue
				}
				podUIDs.Insert(string(pod.UID))
				podVersion := pod.Spec.Containers[0].Env[0].Value
				if podVersion == newVersion {
					continue
				}
				// 如果这是旧版本的 pod 且存在该 pod 的新版本，记录
				// 开始观察到它的时间，否则删除记录（可能节点被排空了）
				if nodesToVersions[pod.Spec.NodeName][newVersion] > 0 {
					if _, ok := nodeToAgeOfOldPod[pod.Spec.NodeName][string(pod.UID)]; !ok {
						if _, ok := nodeToAgeOfOldPod[pod.Spec.NodeName]; !ok {
							nodeToAgeOfOldPod[pod.Spec.NodeName] = make(map[string]time.Time)
						}
						nodeToAgeOfOldPod[pod.Spec.NodeName][string(pod.UID)] = now
					}
				} else {
					delete(nodeToAgeOfOldPod, pod.Spec.NodeName)
				}
			}
			// 从旧 pod 列表中清除所有已删除的 pod
			for node, uidToTime := range nodeToAgeOfOldPod {
				for uid := range uidToTime {
					if !podUIDs.Has(uid) {
						delete(uidToTime, uid)
					}
				}
				if len(uidToTime) == 0 {
					delete(nodeToAgeOfOldPod, node)
				}
			}
			deliberatelyDeletedPods = deliberatelyDeletedPods.Intersection(deletedPodUIDs)

			for _, versions := range nodesToVersions {
				if versions[oldVersion] == 0 {
					nodesWithoutOldVersion++
				}
			}

			var errs []string

			// 不变量：每个节点上最多只能看到一个已删除的 pod，除非出现严重节点问题或 controller 行为异常
			for node, count := range deletedPerNode {
				if count > 1 {
					errs = append(errs, fmt.Sprintf("Node %s has %d deleted pods, which may indicate a problem on the node or a controller race condition", node, count))
				}
			}

			// 不变量：controller 必须在合理时间内（2 倍 grace period）响应新 pod 变为 ready
			for node, uidToTime := range nodeToAgeOfOldPod {
				for uid, firstSeenSinceNewVersionPod := range uidToTime {
					if now.Sub(firstSeenSinceNewVersionPod) > maxSurgeOverlap {
						errs = append(errs, fmt.Sprintf("An old pod with UID %s on a node %s has been running alongside a newer version for longer than %s", uid, node, maxSurgeOverlap))
					}
				}
			}

			// 不变量：新版本未就绪的实例数永远不应超过 maxSurge + oldUnavailableCount，除非
			// 基础设施出现偶发问题，或者是我们故意删除了某个新 pod
			if newUnavailableCount > (maxSurge + oldUnavailableCount + newDeliberatelyDeletedCount + nodesWithoutOldVersion) {
				errs = append(errs, fmt.Sprintf("observed %d new unavailable pods greater than (surge count %d + old unavailable count %d + deliberately deleted new count %d + nodes without old version %d), may be infrastructure flake", newUnavailableCount, maxSurge, oldUnavailableCount, newDeliberatelyDeletedCount, nodesWithoutOldVersion))
			}
			// 不变量：创建的版本总数应为 2
			if versions.Len() > 2 {
				errs = append(errs, fmt.Sprintf("observed %d versions running simultaneously, must have max 2", versions.Len()))
			}
			for _, node := range nodes.List() {
				// 忽略尚未调度的 pod
				if len(node) == 0 {
					continue
				}
				versionCount := make(map[string]int)
				// 不变量：surge 期间每个节点上同一 pod 最多只能有一个实例在运行
				for version, count := range nodesToVersions[node] {
					if count > 1 {
						errs = append(errs, fmt.Sprintf("node %s has %d instances of version %s running simultaneously, must have max 1", node, count, version))
					}
					versionCount[version] += count
				}
				// 不变量：surge 期间允许删除的 pod 最多为 2 个（如果被驱逐）
				for version, count := range nodesToDeletedVersions[node] {
					if count > 2 {
						errs = append(errs, fmt.Sprintf("node %s has %d deleted instances of version %s running simultaneously, must have max 1", node, count, version))
					}
					versionCount[version] += count
				}
				// 不变量：任何节点上同一版本最多只能有两个实例（如果被驱逐）
				for version, count := range versionCount {
					if count > 2 {
						errs = append(errs, fmt.Sprintf("node %s has %d total instances of version %s running simultaneously, must have max 2 (one deleted and one running)", node, count, version))
					}
				}
			}

			if len(errs) > 0 {
				sort.Strings(errs)
				return false, fmt.Errorf("invariants were violated during daemonset update:\n%s", strings.Join(errs, "\n"))
			}

			// 确保节点上每个 daemon pod 都已更新
			nodeNames := e2edaemonset.SchedulableNodes(ctx, c, ds)
			for _, node := range nodeNames {
				switch {
				case
					// 如果还没有新版本
					nodesToVersions[node][newVersion] == 0,
					// 如果某个节点上有多个版本
					len(nodesToVersions[node]) > 1,
					// 如果仍有已删除的 pod
					len(nodesToDeletedVersions[node]) > 0,
					// 如果有新 pod 不可用
					newUnavailableCount > 0:

					// 随机注入故障，确保 controller 能恢复
					switch rand.Intn(25) {
					// 让一个随机的旧 pod 变为未就绪
					case 0:
						// 选择一个未删除的旧版本 pod
						if pod := randomPod(pods, func(pod *v1.Pod) bool {
							return pod.DeletionTimestamp == nil && oldVersion == pod.Spec.Containers[0].Env[0].Value
						}); pod != nil {
							// 将 /tmp/ready 文件设为只读，使 readiness 检查失败
							if _, err := e2ekubectl.RunKubectl(pod.Namespace, "exec", "-c", pod.Spec.Containers[0].Name, pod.Name, "--", "/bin/sh", "-ec", "echo 0 > /var/tmp/ready"); err != nil {
								framework.Logf("Failed to mark pod %s as unready via exec: %v", pod.Name, err)
							} else {
								framework.Logf("Marked old pod %s as unready", pod.Name)
							}
						}
					case 1:
						// 删除一个随机 pod
						if pod := randomPod(pods, func(pod *v1.Pod) bool {
							return pod.DeletionTimestamp == nil
						}); pod != nil {
							if err := c.CoreV1().Pods(ds.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
								framework.Logf("Failed to delete pod %s early: %v", pod.Name, err)
							} else {
								framework.Logf("Deleted pod %s prematurely", pod.Name)
								deliberatelyDeletedPods.Insert(string(pod.UID))
								// 如果是旧版本，不需要测量 controller 的反应时间，因为我们已经手动做了。
								// 如果是新版本，必须重置计时起点，重新计算替代 pod 达到 ready 的时间。
								delete(nodeToAgeOfOldPod, pod.Spec.NodeName)
							}
						}
					}

					// 然后等待
					return false, nil
				}
			}
			return true, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods are still running on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, ds))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")

		// 检查历史和标签
		ds, err = c.AppsV1().DaemonSets(ns).Get(ctx, ds.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		waitForHistoryCreated(ctx, c, ns, label, 2)
		cur = curHistory(listDaemonHistories(ctx, c, ns, label), ds)
		hash = cur.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(cur.Revision).To(gomega.Equal(int64(2)))
		checkDaemonSetPodsLabels(listDaemonPods(ctx, c, ns, label), hash)
	})

	/*
		发布版本：v1.22
		测试名称：DaemonSet, list and delete a collection of DaemonSets
		描述：创建 DaemonSet 必须成功。通过 label selector
		列举 DaemonSet 必须成功。通过 deleteCollection
		删除 DaemonSet 必须成功。
	*/
	framework.ConformanceIt("should list and delete a collection of DaemonSets", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}
		labelSelector := labels.SelectorFromSet(label).String()

		dsClient := f.ClientSet.AppsV1().DaemonSets(ns)
		cs := f.ClientSet
		one := int64(1)

		ginkgo.By(fmt.Sprintf("Creating simple DaemonSet %q", dsName))
		testDaemonset, err := c.AppsV1().DaemonSets(ns).Create(ctx, newDaemonSetWithLabel(dsName, image, label), metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, testDaemonset))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("listing all DaemonSets")
		dsList, err := cs.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Daemon Sets")
		gomega.Expect(dsList.Items).To(gomega.HaveLen(1), "filtered list wasn't found")

		ginkgo.By("DeleteCollection of the DaemonSets")
		err = dsClient.DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: &one}, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to delete DaemonSets")

		ginkgo.By("Verify that ReplicaSets have been deleted")
		dsList, err = c.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list DaemonSets")
		gomega.Expect(dsList.Items).To(gomega.BeEmpty(), "filtered list should have no daemonset")
	})

	/*	发布版本：v1.22
		测试名称：DaemonSet, status sub-resource
		描述：创建 DaemonSet 必须成功。
		尝试读取、更新和 patch 其 status 子资源；所有
		变更子资源的操作必须对后续读取可见。
	*/
	framework.ConformanceIt("should verify changes to a daemon set status", func(ctx context.Context) {
		label := map[string]string{daemonsetNameLabel: dsName}
		labelSelector := labels.SelectorFromSet(label).String()

		dsClient := f.ClientSet.AppsV1().DaemonSets(ns)
		cs := f.ClientSet

		w := &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labelSelector
				return dsClient.Watch(ctx, options)
			},
		}

		dsList, err := cs.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Daemon Sets")

		ginkgo.By(fmt.Sprintf("Creating simple DaemonSet %q", dsName))
		testDaemonset, err := c.AppsV1().DaemonSets(ns).Create(ctx, newDaemonSetWithLabel(dsName, image, label), metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Check that daemon pods launch on every node of the cluster.")
		err = wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkRunningOnAllNodes(f, testDaemonset))
		framework.ExpectNoError(err, "error waiting for daemon pod to start")
		err = e2edaemonset.CheckDaemonStatus(ctx, f, dsName)
		framework.ExpectNoError(err)

		ginkgo.By("Getting /status")
		dsResource := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
		dsStatusUnstructured, err := f.DynamicClient.Resource(dsResource).Namespace(ns).Get(ctx, dsName, metav1.GetOptions{}, "status")
		framework.ExpectNoError(err, "Failed to fetch the status of daemon set %s in namespace %s", dsName, ns)
		dsStatusBytes, err := json.Marshal(dsStatusUnstructured)
		framework.ExpectNoError(err, "Failed to marshal unstructured response. %v", err)

		var dsStatus appsv1.DaemonSet
		err = json.Unmarshal(dsStatusBytes, &dsStatus)
		framework.ExpectNoError(err, "Failed to unmarshal JSON bytes to a daemon set object type")
		framework.Logf("Daemon Set %s has Conditions: %v", dsName, dsStatus.Status.Conditions)

		ginkgo.By("updating the DaemonSet Status")
		var statusToUpdate, updatedStatus *appsv1.DaemonSet

		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			statusToUpdate, err = dsClient.Get(ctx, dsName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Unable to retrieve daemon set %s", dsName)

			statusToUpdate.Status.Conditions = append(statusToUpdate.Status.Conditions, appsv1.DaemonSetCondition{
				Type:    "StatusUpdate",
				Status:  "True",
				Reason:  "E2E",
				Message: "Set from e2e test",
			})

			updatedStatus, err = dsClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err, "Failed to update status. %v", err)
		framework.Logf("updatedStatus.Conditions: %#v", updatedStatus.Status.Conditions)

		ginkgo.By("watching for the daemon set status to be updated")
		ctxUntil, cancel := context.WithTimeout(ctx, dsRetryTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, dsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if ds, ok := event.Object.(*appsv1.DaemonSet); ok {
				found := ds.ObjectMeta.Name == testDaemonset.ObjectMeta.Name &&
					ds.ObjectMeta.Namespace == testDaemonset.ObjectMeta.Namespace &&
					ds.Labels[daemonsetNameLabel] == dsName
				if !found {
					framework.Logf("Observed daemon set %v in namespace %v with annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.Annotations, ds.Status.Conditions)
					return false, nil
				}
				for _, cond := range ds.Status.Conditions {
					if cond.Type == "StatusUpdate" &&
						cond.Reason == "E2E" &&
						cond.Message == "Set from e2e test" {
						framework.Logf("Found daemon set %v in namespace %v with labels: %v annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.ObjectMeta.Labels, ds.Annotations, ds.Status.Conditions)
						return found, nil
					}
					framework.Logf("Observed daemon set %v in namespace %v with annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.Annotations, ds.Status.Conditions)
				}
			}
			object := strings.Split(fmt.Sprintf("%v", event.Object), "{")[0]
			framework.Logf("Observed %v event: %+v", object, event.Type)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate daemon set %v in namespace %v", testDaemonset.ObjectMeta.Name, ns)
		framework.Logf("Daemon set %s has an updated status", dsName)

		ginkgo.By("patching the DaemonSet Status")
		daemonSetStatusPatch := appsv1.DaemonSet{
			Status: appsv1.DaemonSetStatus{
				Conditions: []appsv1.DaemonSetCondition{
					{
						Type:   "StatusPatched",
						Status: "True",
					},
				},
			},
		}

		payload, err := json.Marshal(daemonSetStatusPatch)
		framework.ExpectNoError(err, "Failed to marshal JSON. %v", err)
		_, err = dsClient.Patch(ctx, dsName, types.MergePatchType, payload, metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err, "Failed to patch daemon set status", err)

		ginkgo.By("watching for the daemon set status to be patched")
		ctxUntil, cancel = context.WithTimeout(ctx, dsRetryTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, dsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if ds, ok := event.Object.(*appsv1.DaemonSet); ok {
				found := ds.ObjectMeta.Name == testDaemonset.ObjectMeta.Name &&
					ds.ObjectMeta.Namespace == testDaemonset.ObjectMeta.Namespace &&
					ds.Labels[daemonsetNameLabel] == dsName
				if !found {
					framework.Logf("Observed daemon set %v in namespace %v with annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.Annotations, ds.Status.Conditions)
					return false, nil
				}
				for _, cond := range ds.Status.Conditions {
					if cond.Type == "StatusPatched" {
						framework.Logf("Found daemon set %v in namespace %v with labels: %v annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.ObjectMeta.Labels, ds.Annotations, ds.Status.Conditions)
						return found, nil
					}
					framework.Logf("Observed daemon set %v in namespace %v with annotations: %v & Conditions: %v", ds.ObjectMeta.Name, ds.ObjectMeta.Namespace, ds.Annotations, ds.Status.Conditions)
				}
			}
			object := strings.Split(fmt.Sprintf("%v", event.Object), "{")[0]
			framework.Logf("Observed %v event: %v", object, event.Type)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate daemon set %v in namespace %v", testDaemonset.ObjectMeta.Name, ns)
		framework.Logf("Daemon set %s has a patched status", dsName)
	})
})

// randomPod 从 pod 列表中随机选择一个使 fn 返回 true 的 pod，如果
// 找不到符合条件的 pod 则返回 nil。
func randomPod(pods []v1.Pod, fn func(p *v1.Pod) bool) *v1.Pod {
	podCount := len(pods)
	for offset, i := rand.Intn(podCount), 0; i < (podCount - 1); i++ {
		pod := &pods[(offset+i)%podCount]
		if fn(pod) {
			return pod
		}
	}
	return nil
}

// getDaemonSetImagePatch 生成用于更新 DaemonSet 容器镜像的 patch
func getDaemonSetImagePatch(containerName, containerImage string) string {
	return fmt.Sprintf(`{"spec":{"template":{"spec":{"containers":[{"name":"%s","image":"%s"}]}}}}`, containerName, containerImage)
}

func newDaemonSet(dsName, image string, label map[string]string) *appsv1.DaemonSet {
	ds := newDaemonSetWithLabel(dsName, image, label)
	ds.ObjectMeta.Labels = nil
	return ds
}

func newDaemonSetWithLabel(dsName, image string, label map[string]string) *appsv1.DaemonSet {
	return e2edaemonset.NewDaemonSet(dsName, image, label, nil, nil, []v1.ContainerPort{{ContainerPort: 9376}})
}

func listDaemonPods(ctx context.Context, c clientset.Interface, ns string, label map[string]string) *v1.PodList {
	selector := labels.Set(label).AsSelector()
	options := metav1.ListOptions{
		LabelSelector: selector.String(),
		FieldSelector: nonTerminalPhaseSelector.String(),
	}
	podList, err := c.CoreV1().Pods(ns).List(ctx, options)
	framework.ExpectNoError(err)
	gomega.Expect(podList.Items).ToNot(gomega.BeEmpty())
	return podList
}

func separateDaemonSetNodeLabels(labels map[string]string) (map[string]string, map[string]string) {
	daemonSetLabels := map[string]string{}
	otherLabels := map[string]string{}
	for k, v := range labels {
		if strings.HasPrefix(k, daemonsetLabelPrefix) {
			daemonSetLabels[k] = v
		} else {
			otherLabels[k] = v
		}
	}
	return daemonSetLabels, otherLabels
}

func clearDaemonSetNodeLabels(ctx context.Context, c clientset.Interface) error {
	nodeList, err := e2enode.GetReadySchedulableNodes(ctx, c)
	if err != nil {
		return err
	}
	for _, node := range nodeList.Items {
		_, err := setDaemonSetNodeLabels(ctx, c, node.Name, map[string]string{})
		if err != nil {
			return err
		}
	}
	return nil
}

// patchNamespaceAnnotations 将测试 namespace 上与 node selector 相关的注解置空
func patchNamespaceAnnotations(ctx context.Context, c clientset.Interface, nsName string) (*v1.Namespace, error) {
	nsClient := c.CoreV1().Namespaces()

	annotations := make(map[string]string)
	for _, n := range NamespaceNodeSelectors {
		annotations[n] = ""
	}
	nsPatch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	})
	if err != nil {
		return nil, err
	}

	return nsClient.Patch(ctx, nsName, types.StrategicMergePatchType, nsPatch, metav1.PatchOptions{})
}

func setDaemonSetNodeLabels(ctx context.Context, c clientset.Interface, nodeName string, labels map[string]string) (*v1.Node, error) {
	nodeClient := c.CoreV1().Nodes()
	var newNode *v1.Node
	var newLabels map[string]string
	err := wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, func(ctx context.Context) (bool, error) {
		node, err := nodeClient.Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		// 移除本测试创建的所有标签
		daemonSetLabels, otherLabels := separateDaemonSetNodeLabels(node.Labels)
		if reflect.DeepEqual(daemonSetLabels, labels) {
			newNode = node
			return true, nil
		}
		node.Labels = otherLabels
		for k, v := range labels {
			node.Labels[k] = v
		}
		newNode, err = nodeClient.Update(ctx, node, metav1.UpdateOptions{})
		if err == nil {
			newLabels, _ = separateDaemonSetNodeLabels(newNode.Labels)
			return true, err
		}
		if se, ok := err.(*apierrors.StatusError); ok && se.ErrStatus.Reason == metav1.StatusReasonConflict {
			framework.Logf("failed to update node due to resource version conflict")
			return false, nil
		}
		return false, err
	})
	if err != nil {
		return nil, err
	} else if len(newLabels) != len(labels) {
		return nil, fmt.Errorf("could not set daemon set test labels as expected")
	}

	return newNode, nil
}

func checkRunningOnAllNodes(f *framework.Framework, ds *appsv1.DaemonSet) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return e2edaemonset.CheckRunningOnAllNodes(ctx, f, ds)
	}
}

func checkAtLeastOneNewPod(c clientset.Interface, ns string, label map[string]string, newImage string) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		pods := listDaemonPods(ctx, c, ns, label)
		for _, pod := range pods.Items {
			if pod.Spec.Containers[0].Image == newImage {
				return true, nil
			}
		}
		return false, nil
	}
}

func checkRunningOnNoNodes(f *framework.Framework, ds *appsv1.DaemonSet) func(ctx context.Context) (bool, error) {
	return e2edaemonset.CheckDaemonPodOnNodes(f, ds, make([]string, 0))
}

func checkDaemonPodsImageAndAvailability(c clientset.Interface, ds *appsv1.DaemonSet, image string, maxUnavailable int) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		podList, err := c.CoreV1().Pods(ds.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		pods := podList.Items

		unavailablePods := 0
		nodesToUpdatedPodCount := make(map[string]int)
		for _, pod := range pods {
			// 忽略节点上预期会被删除的 pod
			if pod.DeletionTimestamp != nil {
				continue
			}
			if !metav1.IsControlledBy(&pod, ds) {
				continue
			}
			podImage := pod.Spec.Containers[0].Image
			if podImage != image {
				framework.Logf("Wrong image for pod: %s. Expected: %s, got: %s.", pod.Name, image, podImage)
			} else {
				nodesToUpdatedPodCount[pod.Spec.NodeName]++
			}
			if !podutil.IsPodAvailable(&pod, ds.Spec.MinReadySeconds, metav1.Now()) {
				framework.Logf("Pod %s is not available", pod.Name)
				unavailablePods++
			}
		}
		if unavailablePods > maxUnavailable {
			return false, fmt.Errorf("number of unavailable pods: %d is greater than maxUnavailable: %d", unavailablePods, maxUnavailable)
		}
		// 确保节点上每个 daemon pod 都已更新
		nodeNames := e2edaemonset.SchedulableNodes(ctx, c, ds)
		for _, node := range nodeNames {
			if nodesToUpdatedPodCount[node] == 0 {
				return false, nil
			}
		}
		return true, nil
	}
}

func checkDaemonSetPodsLabels(podList *v1.PodList, hash string) {
	for _, pod := range podList.Items {
		// 忽略所有将被删除的 DS pod
		if pod.DeletionTimestamp != nil {
			continue
		}
		podHash := pod.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]
		gomega.Expect(podHash).ToNot(gomega.BeEmpty())
		if len(hash) > 0 {
			gomega.Expect(podHash).To(gomega.Equal(hash), "unexpected hash for pod %s", pod.Name)
		}
	}
}

func waitForHistoryCreated(ctx context.Context, c clientset.Interface, ns string, label map[string]string, numHistory int) {
	listHistoryFn := func(ctx context.Context) (bool, error) {
		selector := labels.Set(label).AsSelector()
		options := metav1.ListOptions{LabelSelector: selector.String()}
		historyList, err := c.AppsV1().ControllerRevisions(ns).List(ctx, options)
		if err != nil {
			return false, err
		}
		if len(historyList.Items) == numHistory {
			return true, nil
		}
		framework.Logf("%d/%d controllerrevisions created.", len(historyList.Items), numHistory)
		return false, nil
	}
	err := wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, listHistoryFn)
	framework.ExpectNoError(err, "error waiting for controllerrevisions to be created")
}

func listDaemonHistories(ctx context.Context, c clientset.Interface, ns string, label map[string]string) *appsv1.ControllerRevisionList {
	selector := labels.Set(label).AsSelector()
	options := metav1.ListOptions{LabelSelector: selector.String()}
	historyList, err := c.AppsV1().ControllerRevisions(ns).List(ctx, options)
	framework.ExpectNoError(err)
	gomega.Expect(historyList.Items).ToNot(gomega.BeEmpty())
	return historyList
}

func curHistory(historyList *appsv1.ControllerRevisionList, ds *appsv1.DaemonSet) *appsv1.ControllerRevision {
	var curHistory *appsv1.ControllerRevision
	foundCurHistories := 0
	for i := range historyList.Items {
		history := &historyList.Items[i]
		// 每个 history 都应有 hash 标签
		gomega.Expect(history.Labels[appsv1.DefaultDaemonSetUniqueLabelKey]).ToNot(gomega.BeEmpty())
		match, err := daemon.Match(ds, history)
		framework.ExpectNoError(err)
		if match {
			curHistory = history
			foundCurHistories++
		}
	}
	gomega.Expect(foundCurHistories).To(gomega.Equal(1))
	gomega.Expect(curHistory).NotTo(gomega.BeNil())
	return curHistory
}

func waitFailedDaemonPodDeleted(c clientset.Interface, pod *v1.Pod) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		if _, err := c.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, fmt.Errorf("failed to get failed daemon pod %q: %w", pod.Name, err)
		}
		return false, nil
	}
}
