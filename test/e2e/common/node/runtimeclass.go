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

package node

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/kubernetes/pkg/kubelet/events"
	runtimeclasstest "k8s.io/kubernetes/pkg/kubelet/runtimeclass/testing"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eevents "k8s.io/kubernetes/test/e2e/framework/events"
	e2eruntimeclass "k8s.io/kubernetes/test/e2e/framework/node/runtimeclass"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("RuntimeClass", func() {
	f := framework.NewDefaultFramework("runtimeclass")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	/*
		Release: v1.20
		Testname: Pod with the non-existing RuntimeClass is rejected.
		Description: 请求不存在的 RuntimeClass 的 Pod 必须被拒绝。
	*/
	framework.ConformanceIt("should reject a Pod requesting a non-existent RuntimeClass", f.WithNodeConformance(), func(ctx context.Context) {
		rcName := f.Namespace.Name + "-nonexistent"
		err := expectPodRejection(ctx, f, rcName)
		// 预期上面的 Pod 被拒绝。
		// 如果得到的错误不是拒绝错误，则测试失败。
		framework.ExpectNoError(err, "unexpected error for pod rejection")
	})
	// 该测试不能成为 Conformance，因为它依赖容器运行时未安装特定 handler。
	f.It("should reject a Pod requesting a RuntimeClass with an unconfigured handler", feature.RuntimeHandler, func(ctx context.Context) {
		handler := f.Namespace.Name + "-handler"
		rcName := createRuntimeClass(ctx, f, "unconfigured-handler", handler, nil)
		ginkgo.DeferCleanup(deleteRuntimeClass, f, rcName)
		pod := e2epod.NewPodClient(f).Create(ctx, e2eruntimeclass.NewRuntimeClassPod(rcName))
		eventSelector := fields.Set{
			"involvedObject.kind":      "Pod",
			"involvedObject.name":      pod.Name,
			"involvedObject.namespace": f.Namespace.Name,
			"reason":                   events.FailedCreatePodSandBox,
		}.AsSelector().String()
		// 事件不可靠，不能依赖事件，这里只用来加速测试。
		err := e2eevents.WaitTimeoutForEvent(ctx, f.ClientSet, f.Namespace.Name, eventSelector, handler, framework.PodEventTimeout)
		if err != nil {
			framework.Logf("Warning: did not get event about FailedCreatePodSandBox. Err: %v", err)
		}
		// 检查 Pod 仍未运行
		p, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "could not re-read the pod after event (or timeout)")
		gomega.Expect(p.Status.Phase).To(gomega.Equal(v1.PodPending), "Pod phase isn't pending")
	})

	// 本测试要求节点上已经配置好 PreconfiguredRuntimeClassHandler。
	// 该测试不能成为 Conformance，因为它依赖容器运行时安装并可用特定 handler。
	f.It("should run a Pod requesting a RuntimeClass with a configured handler", feature.RuntimeHandler, func(ctx context.Context) {
		if err := e2eruntimeclass.NodeSupportsPreconfiguredRuntimeClassHandler(ctx, f); err != nil {
			e2eskipper.Skipf("Skipping test as node does not have E2E runtime class handler preconfigured in container runtime config: %v", err)
		}

		rcName := createRuntimeClass(ctx, f, "preconfigured-handler", e2eruntimeclass.PreconfiguredRuntimeClassHandler, nil)
		ginkgo.DeferCleanup(deleteRuntimeClass, f, rcName)
		pod := e2epod.NewPodClient(f).Create(ctx, e2eruntimeclass.NewRuntimeClassPod(rcName))
		expectPodSuccess(ctx, f, pod)
	})

	/*
		Release: v1.20
		Testname: Can schedule a pod requesting existing RuntimeClass.
		Description: 请求已有 RuntimeClass 的 Pod 必须能被调度。
		本测试不校验 Pod 是否真的能启动，因为该能力
		取决于容器运行时和预配置的 handler。运行时特定功能
		不在此测试范围内。
	*/
	framework.ConformanceIt("should schedule a Pod requesting a RuntimeClass without PodOverhead", f.WithNodeConformance(), func(ctx context.Context) {
		rcName := createRuntimeClass(ctx, f, "preconfigured-handler", e2eruntimeclass.PreconfiguredRuntimeClassHandler, nil)
		ginkgo.DeferCleanup(deleteRuntimeClass, f, rcName)
		pod := e2epod.NewPodClient(f).Create(ctx, e2eruntimeclass.NewRuntimeClassPod(rcName))
		// 命名空间中只有一个 Pod
		label := labels.SelectorFromSet(labels.Set(map[string]string{}))
		pods, err := e2epod.WaitForPodsWithLabelScheduled(ctx, f.ClientSet, f.Namespace.Name, label)
		framework.ExpectNoError(err, "Failed to schedule Pod with the RuntimeClass")

		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		scheduledPod := &pods.Items[0]
		gomega.Expect(scheduledPod.Name).To(gomega.Equal(pod.Name))

		// 不应设置 Overhead
		gomega.Expect(scheduledPod.Spec.Overhead).To(gomega.BeEmpty())
	})

	/*
		Release: v1.24
		Testname: RuntimeClass Overhead field must be respected.
		Description: 请求已有 RuntimeClass 的 Pod 必须能被调度。
		本测试不校验 Pod 是否真的能启动，因为该能力
		取决于容器运行时和预配置的 handler。运行时特定功能
		不在此测试范围内。
	*/
	framework.ConformanceIt("should schedule a Pod requesting a RuntimeClass and initialize its Overhead", f.WithNodeConformance(), func(ctx context.Context) {
		rcName := createRuntimeClass(ctx, f, "preconfigured-handler", e2eruntimeclass.PreconfiguredRuntimeClassHandler, &nodev1.Overhead{
			PodFixed: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10m"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("1Mi"),
			},
		})
		ginkgo.DeferCleanup(deleteRuntimeClass, f, rcName)
		pod := e2epod.NewPodClient(f).Create(ctx, e2eruntimeclass.NewRuntimeClassPod(rcName))
		// 命名空间中只有一个 Pod
		label := labels.SelectorFromSet(labels.Set(map[string]string{}))
		pods, err := e2epod.WaitForPodsWithLabelScheduled(ctx, f.ClientSet, f.Namespace.Name, label)
		framework.ExpectNoError(err, "Failed to schedule Pod with the RuntimeClass")

		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		scheduledPod := &pods.Items[0]
		gomega.Expect(scheduledPod.Name).To(gomega.Equal(pod.Name))

		gomega.Expect(scheduledPod.Spec.Overhead[v1.ResourceCPU]).To(gomega.Equal(resource.MustParse("10m")))
		gomega.Expect(scheduledPod.Spec.Overhead[v1.ResourceMemory]).To(gomega.Equal(resource.MustParse("1Mi")))
	})

	/*
		Release: v1.20
		Testname: Pod with the deleted RuntimeClass is rejected.
		Description: 请求已删除 RuntimeClass 的 Pod 必须被拒绝。
	*/
	framework.ConformanceIt("should reject a Pod requesting a deleted RuntimeClass", f.WithNodeConformance(), func(ctx context.Context) {
		rcName := createRuntimeClass(ctx, f, "delete-me", "runc", nil)
		rcClient := f.ClientSet.NodeV1().RuntimeClasses()

		ginkgo.By("Deleting RuntimeClass "+rcName, func() {
			deleteRuntimeClass(ctx, f, rcName)

			ginkgo.By("Waiting for the RuntimeClass to disappear")
			framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, framework.Poll, time.Minute, true, func(ctx context.Context) (bool, error) {
				_, err := rcClient.Get(ctx, rcName, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil // 完成
				}
				if err != nil {
					return true, err // 带错误停止等待
				}
				return false, nil
			}))
		})

		gomega.Eventually(ctx, func() error {
			return expectPodRejection(ctx, f, rcName)
		}, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.Succeed())
	})

	/*
		Release: v1.20
		Testname: RuntimeClass API
		Description:
		The node.k8s.io API 组必须出现在 /apis discovery 文档中。
		The node.k8s.io/v1 API 组版本必须出现在 /apis/mode.k8s.io discovery 文档中。
		runtimeclasses 资源必须出现在 /apis/node.k8s.io/v1 discovery 文档中。
		runtimeclasses 资源必须支持 create、get、list、watch、update、patch、delete 和 deletecollection。
	*/
	framework.ConformanceIt("should support RuntimeClasses API operations", func(ctx context.Context) {
		// 环境准备
		rcVersion := "v1"
		rcClient := f.ClientSet.NodeV1().RuntimeClasses()

		// 这是 conformance 测试，必须配置 opaque handler 来校验 CRUD 操作。
		// 测试不应使用任何已有的 handler，例如 gVisor 或 runc。
		//
		// 本测试中所有 CRUD 操作仅限于带 label test=f.UniqueName 的对象
		rc := runtimeclasstest.NewRuntimeClass(f.UniqueName+"-handler", f.UniqueName+"-conformance-runtime-class")
		rc.SetLabels(map[string]string{"test": f.UniqueName})
		rc2 := runtimeclasstest.NewRuntimeClass(f.UniqueName+"-handler2", f.UniqueName+"-conformance-runtime-class2")
		rc2.SetLabels(map[string]string{"test": f.UniqueName})
		rc3 := runtimeclasstest.NewRuntimeClass(f.UniqueName+"-handler3", f.UniqueName+"-conformance-runtime-class3")
		rc3.SetLabels(map[string]string{"test": f.UniqueName})

		// discovery 检查

		ginkgo.By("getting /apis")
		{
			discoveryGroups, err := f.ClientSet.Discovery().ServerGroups()
			framework.ExpectNoError(err)
			found := false
			for _, group := range discoveryGroups.Groups {
				if group.Name == nodev1.GroupName {
					for _, version := range group.Versions {
						if version.Version == rcVersion {
							found = true
							break
						}
					}
				}
			}
			if !found {
				framework.Failf("expected RuntimeClass API group/version, got %#v", discoveryGroups.Groups)
			}
		}

		ginkgo.By("getting /apis/node.k8s.io")
		{
			group := &metav1.APIGroup{}
			err := f.ClientSet.Discovery().RESTClient().Get().AbsPath("/apis/node.k8s.io").Do(ctx).Into(group)
			framework.ExpectNoError(err)
			found := false
			for _, version := range group.Versions {
				if version.Version == rcVersion {
					found = true
					break
				}
			}
			if !found {
				framework.Failf("expected RuntimeClass API version, got %#v", group.Versions)
			}
		}

		ginkgo.By("getting /apis/node.k8s.io/" + rcVersion)
		{
			resources, err := f.ClientSet.Discovery().ServerResourcesForGroupVersion(nodev1.SchemeGroupVersion.String())
			framework.ExpectNoError(err)
			found := false
			for _, resource := range resources.APIResources {
				switch resource.Name {
				case "runtimeclasses":
					found = true
				}
			}
			if !found {
				framework.Failf("expected runtimeclasses, got %#v", resources.APIResources)
			}
		}

		// 主资源的 create/read/update/watch 操作

		ginkgo.By("creating")
		createdRC, err := rcClient.Create(ctx, rc, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		_, err = rcClient.Create(ctx, rc, metav1.CreateOptions{})
		if !apierrors.IsAlreadyExists(err) {
			framework.Failf("expected 409, got %#v", err)
		}
		_, err = rcClient.Create(ctx, rc2, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("watching")
		framework.Logf("starting watch")
		rcWatch, err := rcClient.Watch(ctx, metav1.ListOptions{LabelSelector: "test=" + f.UniqueName})
		framework.ExpectNoError(err)

		// 为 watch 添加
		_, err = rcClient.Create(ctx, rc3, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("getting")
		gottenRC, err := rcClient.Get(ctx, rc.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(gottenRC.UID).To(gomega.Equal(createdRC.UID))
		gomega.Expect(gottenRC).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("listing")
		rcs, err := rcClient.List(ctx, metav1.ListOptions{LabelSelector: "test=" + f.UniqueName})
		framework.ExpectNoError(err)
		gomega.Expect(rcs.Items).To(gomega.HaveLen(3), "filtered list should have 3 items")

		ginkgo.By("patching")
		patchedRC, err := rcClient.Patch(ctx, createdRC.Name, types.MergePatchType, []byte(`{"metadata":{"annotations":{"patched":"true"}}}`), metav1.PatchOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(patchedRC.Annotations).To(gomega.HaveKeyWithValue("patched", "true"), "patched object should have the applied annotation")
		gomega.Expect(resourceversion.CompareResourceVersion(gottenRC.ResourceVersion, patchedRC.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("updating")
		csrToUpdate := patchedRC.DeepCopy()
		csrToUpdate.Annotations["updated"] = "true"
		updatedRC, err := rcClient.Update(ctx, csrToUpdate, metav1.UpdateOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(updatedRC.Annotations).To(gomega.HaveKeyWithValue("updated", "true"), "updated object should have the applied annotation")

		framework.Logf("waiting for watch events with expected annotations")
		for sawAdded, sawPatched, sawUpdated := false, false, false; !sawAdded && !sawPatched && !sawUpdated; {
			select {
			case evt, ok := <-rcWatch.ResultChan():
				if !ok {
					framework.Fail("watch channel should not close")
				}
				if evt.Type == watch.Modified {
					watchedRC, isRC := evt.Object.(*nodev1.RuntimeClass)
					if !isRC {
						framework.Failf("expected RC, got %T", evt.Object)
					}
					if watchedRC.Annotations["patched"] == "true" {
						framework.Logf("saw patched annotations")
						sawPatched = true
					} else if watchedRC.Annotations["updated"] == "true" {
						framework.Logf("saw updated annotations")
						sawUpdated = true
					} else {
						framework.Logf("missing expected annotations, waiting: %#v", watchedRC.Annotations)
					}
				} else if evt.Type == watch.Added {
					_, isRC := evt.Object.(*nodev1.RuntimeClass)
					if !isRC {
						framework.Failf("expected RC, got %T", evt.Object)
					}
					sawAdded = true
				}

			case <-time.After(wait.ForeverTestTimeout):
				framework.Fail("timed out waiting for watch event")
			}
		}
		rcWatch.Stop()

		// 主资源删除操作

		ginkgo.By("deleting")
		err = rcClient.Delete(ctx, createdRC.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)
		_, err = rcClient.Get(ctx, createdRC.Name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			framework.Failf("expected 404, got %#v", err)
		}
		rcs, err = rcClient.List(ctx, metav1.ListOptions{LabelSelector: "test=" + f.UniqueName})
		framework.ExpectNoError(err)
		gomega.Expect(rcs.Items).To(gomega.HaveLen(2), "filtered list should have 2 items")

		ginkgo.By("deleting a collection")
		err = rcClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "test=" + f.UniqueName})
		framework.ExpectNoError(err)
		rcs, err = rcClient.List(ctx, metav1.ListOptions{LabelSelector: "test=" + f.UniqueName})
		framework.ExpectNoError(err)
		gomega.Expect(rcs.Items).To(gomega.BeEmpty(), "filtered list should have 0 items")
	})
})

func deleteRuntimeClass(ctx context.Context, f *framework.Framework, name string) {
	err := f.ClientSet.NodeV1().RuntimeClasses().Delete(ctx, name, metav1.DeleteOptions{})
	framework.ExpectNoError(err, "failed to delete RuntimeClass resource")
}

// createRuntimeClass 生成带有指定 handler 和 "namespaced" 名称的 RuntimeClass，
// 同步创建它并返回生成的名称。
func createRuntimeClass(ctx context.Context, f *framework.Framework, name, handler string, overhead *nodev1.Overhead) string {
	uniqueName := fmt.Sprintf("%s-%s", f.Namespace.Name, name)
	rc := runtimeclasstest.NewRuntimeClass(uniqueName, handler)
	rc.Overhead = overhead
	rc, err := f.ClientSet.NodeV1().RuntimeClasses().Create(ctx, rc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create RuntimeClass resource")
	return rc.GetName()
}

// expectPodRejection 测试 kubelet 无法接纳 runtime class Pod
// 拒绝发生在准入阶段，这些场景预期会得到 forbidden 错误。
func expectPodRejection(ctx context.Context, f *framework.Framework, rcName string) error {
	pod := e2eruntimeclass.NewRuntimeClassPod(rcName)
	_, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		return fmt.Errorf("should get forbidden error")
	}
	if !apierrors.IsForbidden(err) {
		return err
	}
	// 本例中意味着收到了 forbidden 错误
	return nil
}

// expectPodSuccess 等待给定 Pod 成功终止。
func expectPodSuccess(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespace(
		ctx, f.ClientSet, pod.Name, f.Namespace.Name))
}
